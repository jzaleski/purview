module Purview
  module Mixins
    module Dialect
      def dialect
        dialect_type.new
      end

      def false_value
        dialect.false_value
      end

      def null_value
        dialect.null_value
      end

      def quoted(value)
        dialect.quoted(value)
      end

      def sanitized(value)
        dialect.sanitized(value)
      end

      def true_value
        dialect.true_value
      end
    end
  end
end
