module Purview
  module Structs
    class TableMetadata < Base
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
