module Purview
  module Loaders
    class Base
      def initialize(opts={})
        @opts = opts
      end

      def load(connection, rows, window)
        raise %{All "#{Base}(s)" must override the "load" method}
      end

      private

      include Purview::Mixins::Logger

      attr_reader :opts

      def database
        table.database
      end

      def quoted(value)
        database.quoted(value)
      end

      def row_values(row)
        table.column_names.map { |column_name| quoted(row[column_name]) }.join(', ')
      end

      def table
        opts[:table]
      end
    end
  end
end
