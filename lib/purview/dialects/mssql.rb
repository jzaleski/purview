module Purview
  module Dialects
    class MSSQL < Base
      def false_value
        '0'
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
        '1'
      end
    end
  end
end
