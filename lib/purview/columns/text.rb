module Purview
  module Columns
    class Text < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::String)
      end
    end
  end
end
