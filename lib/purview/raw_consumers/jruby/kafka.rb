safe_require('jruby-kafka')

if defined?(Kafka)
  module Purview
    module RawConsumers
      module JRuby
        class Kafka < Base
          # TODO: Implement me!
        end
      end
    end
  end

  Purview::RawConsumers::Kafka = Purview::RawConsumers::JRuby::Kafka
end
