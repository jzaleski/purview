module Purview
  module Structs
    class TableMetadata < Base
      def initialize(row)
        enabled_at = row.enabled_at && Time.parse(row.enabled_at)
        last_pulled_at = row.last_pulled_at && Time.parse(row.last_pulled_at)
        locked_at = row.locked_at && Time.parse(row.locked_at)
        max_timestamp_pulled = row.max_timestamp_pulled && Time.parse(row.max_timestamp_pulled)
        super(
          :table_name => row.table_name,
          :enabled_at => enabled_at,
          :last_pulled_at => last_pulled_at,
          :locked_at => locked_at,
          :max_timestamp_pulled => max_timestamp_pulled,
        )
      end

      def diabled?
        !enabled?
      end

      def enabled?
        !!enabled_at
      end

      def initialized?
        !!max_timestamp_pulled
      end

      def locked?
        !!locked_at
      end

      def unlocked?
        !locked?
      end
    end
  end
end
