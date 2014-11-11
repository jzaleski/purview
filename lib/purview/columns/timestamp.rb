module Purview
  module Columns
    class Timestamp < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Time)
      end
    end
  end
end
