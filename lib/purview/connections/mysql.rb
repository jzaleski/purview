module Purview
  module Connections
    class MySQL < Base
      private

      def raw_connection_type
        Purview::RawConnections::MySQL
      end
    end
  end
end
