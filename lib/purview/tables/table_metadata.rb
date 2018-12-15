module Purview
  module Tables
    class TableMetadata < Base
      def initialize(database)
        super(
          :table_metadata,
          :columns => [
            table_name_column,
            enabled_at_column,
            last_pulled_at_column,
            last_updated_at_column,
            locked_at_column,
            max_timestamp_pulled_column,
          ],
          :database => database
        )
      end

      def enabled_at_column
        Purview::Columns::Timestamp.new(:enabled_at)
      end

      def last_pulled_at_column
        Purview::Columns::Timestamp.new(:last_pulled_at)
      end

      def last_updated_at_column
        Purview::Columns::Timestamp.new(:last_updated_at)
      end

      def locked_at_column
        Purview::Columns::Timestamp.new(:locked_at)
      end

      def max_timestamp_pulled_column
        Purview::Columns::Timestamp.new(:max_timestamp_pulled)
      end

      def table_name_column
        Purview::Columns::Id.new(
          :table_name,
          :type => Purview::Types::String,
          :limit => 255,
        )
      end
    end
  end
end
