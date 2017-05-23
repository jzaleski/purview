safe_require('bunny')

if defined?(Bunny)
  module Purview
    module RawConsumers
      class Bunny < Base
        # TODO: Implement me!
      end
    end
  end

  Purview::RawConsumers::RabbitMQ = Purview::RawConsumers::Bunny
end
