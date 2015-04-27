module Purview
  module Columns
    class UUID < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::UUID, :limit => 36)
      end
    end
  end
end
