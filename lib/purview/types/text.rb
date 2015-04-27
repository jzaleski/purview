module Purview
  module Types
    class Text < Base
      def self.parse(value)
        String(value)
      end
    end
  end
end
