module Purview
  module Columns
    class Boolean < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Boolean)
      end
    end
  end
end
