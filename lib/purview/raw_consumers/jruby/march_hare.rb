safe_require('march_hare')

if defined?(MarchHare)
  module Purview
    module RawConsumers
      module JRuby
        class MarchHare < Base
          # TODO: Implement me!
        end
      end
    end
  end

  Purview::RawConsumers::RabbitMQ = Purview::RawConsumers::JRuby::MarchHare
end
