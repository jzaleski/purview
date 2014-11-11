module Purview
  module Columns
    class String < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::String, :limit => 255)
      end
    end
  end
end
