module Purview
  module Types
    class UUID < Base
      def self.parse(value)
        String(value)
      end
    end
  end
end
