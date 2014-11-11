module Purview
  module Columns
    class Time < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Time)
      end
    end
  end
end
