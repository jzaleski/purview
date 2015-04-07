module Purview
  module Databases
    class MySQL < Base
      private

      def connection_opts
        { :database => name }
      end

      def connection_type
        Purview::Connections::MySQL
      end

      def database_type_map
        {
          Purview::Columns::Boolean => 'boolean',
          Purview::Columns::Date => 'date',
          Purview::Columns::Float => 'float',
          Purview::Columns::Integer => 'integer',
          Purview::Columns::Money => 'decimal',
          Purview::Columns::String => 'varchar',
          Purview::Columns::Text => 'text',
          Purview::Columns::Time => 'time',
          Purview::Columns::Timestamp => 'timestamp',
          Purview::Columns::UUID => 'varchar',
        }
      end
    end
  end
end
