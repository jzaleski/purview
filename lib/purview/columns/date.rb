module Purview
  module Columns
    class Date < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Date)
      end
    end
  end
end
