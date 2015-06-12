module Purview
  module Connections
    class MSSQL < Base
      private

      def raw_connection_type
        Purview::RawConnections::MSSQL
      end
    end
  end
end
