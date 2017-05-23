module Purview
  module Consumers
    class RabbitMQ < Base
      private

      def raw_consumer_type
        Purview::RawConsumers::RabbitMQ
      end
    end
  end
end
