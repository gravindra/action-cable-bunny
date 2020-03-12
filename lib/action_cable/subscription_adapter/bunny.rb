# frozen_string_literal: true

require "bunny"
require "securerandom"
module ActionCable
  module SubscriptionAdapter
    class Bunny < Base # :nodoc:
      prepend ChannelPrefix

      attr_reader :connection, :channel, :exchange

      def initialize(*)
        super
        @listener = nil
        @connection = ::Bunny.new(ENV["CLOUDAMQP_URL"]).tap(&:start)
        @channel = @connection.create_channel
      end

      def broadcast(channel, payload)
        exchange = @channel.fanout(channel_identifier(channel), auto_delete: false)
        exchange.publish(payload.to_json)
      end

      def subscribe(channel, callback, success_callback = nil)
        listener.add_subscriber(channel_identifier(channel), callback, success_callback)
      end

      def unsubscribe(channel, callback)
        listener.remove_subscriber(channel_identifier(channel), callback)
      end

      def shutdown
        listener.shutdown
      end

      private

        def channel_identifier(channel)
          "actioncable.#{channel}"
        end

        def listener
          @listener || @server.mutex.synchronize { @listener ||= Listener.new(self, @server.event_loop) }
        end

        class Listener < SubscriberMap
          class ShutdownThread < StandardError; end
          attr_reader :logger
          def initialize(adapter, event_loop)
            super()
            @adapter = adapter
            @logger = adapter.logger
            @event_loop = event_loop
            @queue = Queue.new
            @consumer = nil
            @thread = Thread.new do
              Thread.current.abort_on_exception = true
              listen
            end
          end

          def listen
            catch ShutdownThread do
              loop do
                queue_next until @queue.empty?
              end
            end
          end

          def add_channel(channel, on_success)
            @queue.push([:listen, channel, on_success])
          end

          def remove_channel(channel, on_success=nil)
            @queue.push([:unlisten, channel, on_success])
          end

          def shutdown
            @queue.push([:shutdown])
            Thread.pass while @thread.alive?
          end

          private

            def queue_next
              action, channel, callback = @queue.pop(true)
              consumer_unsubscribe(channel) if %i[unlisten shutdown].include?(action)
              consumer_subscribe(channel) if action == :listen
              throw ShutdownThread if action == :shutdown
              callback&.call
            end

            def consumer_subscribe(channel)
              channel_name = "#{channel}"
              exchange = @adapter.channel.fanout(channel, auto_delete: false)

              queue = @adapter
                      .channel
                      .queue(channel_name, :durable => true, :auto_delete => false)
                      .bind(exchange)
              @consumer = queue.subscribe(:manual_ack => true) do |_d, _m, p|
                broadcast(channel, p)
                @adapter.channel.ack(_d.delivery_tag)
              end
            end

            def consumer_unsubscribe(_channel)
              @consumer&.cancel
            end
        end
    end
  end
end
