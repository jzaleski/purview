module Purview
  module Dialects
    class PostgreSQL < Base
      def false_value
        'FALSE'
      end

      def null_value
        'NULL'
      end

      def quoted(value)
        value.nil? ? null_value : value.quoted
      end

      def sanitized(value)
        value.nil? ? null_value : value.sanitized
      end

      def true_value
        'TRUE'
      end
    end
  end
end
