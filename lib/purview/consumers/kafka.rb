module Purview
  module Consumers
    class Kafka < Base
      private

      def raw_consumer_type
        Purview::RawConsumers::Kafka
      end
    end
  end
end
