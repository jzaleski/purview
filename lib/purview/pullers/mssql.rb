module Purview
  module Pullers
    class MSSQL < BaseSQL
      private

      def connection_type
        Purview::Connections::MSSQL
      end

      def dialect_type
        Purview::Dialects::MSSQL
      end
    end
  end
end
