module Purview
  module Pullers
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def pull(window)
        raise %{All "#{Base}(s)" must override the "pull" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts

      def table
        opts[:table]
      end
    end
  end
end
