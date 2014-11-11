module Purview
  module Columns
    class Integer < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Integer)
      end
    end
  end
end
