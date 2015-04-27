module Purview
  module Columns
    class Float < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Float)
      end
    end
  end
end
