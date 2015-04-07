module Purview
  module Columns
    class Timestamp < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Timestamp)
      end
    end
  end
end
