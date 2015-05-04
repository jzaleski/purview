module Purview
  module Pullers
    class MySQL < BaseSQL
      private

      def connection_type
        Purview::Connections::MySQL
      end

      def null_value
        'NULL'
      end
    end
  end
end
