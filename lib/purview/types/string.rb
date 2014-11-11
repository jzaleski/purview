module Purview
  module Types
    class String < Base
      def self.parse(value)
        String(value)
      end
    end
  end
end
