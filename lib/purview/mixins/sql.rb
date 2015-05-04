module Purview
  module Mixins
    module SQL
      def quoted(value, default=null_value)
        value.nil? ? default : value.quoted
      end

      def sanitized(value, default=null_value)
        value.nil? ? default : value.sanitized
      end
    end
  end
end
