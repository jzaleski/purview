module Purview
  module Pullers
    class MySQL < BaseSQL
      private

      def connection_type
        Purview::Connections::MySQL
      end

      def dialect_type
        Purview::Dialects::MySQL
      end
    end
  end
end
