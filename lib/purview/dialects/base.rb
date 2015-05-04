module Purview
  module Dialects
    class Base
      def false_value
        raise %{All "#{Base}(s)" must override the "false_value" method}
      end

      def null_value
        raise %{All "#{Base}(s)" must override the "null_value" method}
      end

      def quoted(value)
        raise %{All "#{Base}(s)" must override the "quoted" method}
      end

      def sanitized(value)
        raise %{All "#{Base}(s)" must override the "sanitized" method}
      end

      def true_value
        raise %{All "#{Base}(s)" must override the "true_value" method}
      end
    end
  end
end
