module Purview
  module Mixins
    module Helpers
      def blank?(value)
        value.to_s.strip.length.zero?
      end

      def coalesce(value, default)
        value.nil? ? default : value
      end

      def present?(value)
        !blank?(value)
      end

      def zero?(value)
        Integer(value).zero?
      end
    end
  end
end
