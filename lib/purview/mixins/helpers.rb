module Purview
  module Mixins
    module Helpers
      def blank?(value)
        value.to_s.strip.length.zero?
      end

      def coalesced(value, default)
        value.nil? ? default : value
      end

      def filter_nil_values(hash)
        hash.reject { |_, value| value.nil? }
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
