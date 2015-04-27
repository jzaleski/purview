module Purview
  module Types
    class Money < Base
      def self.parse(value)
        Float(value)
      end
    end
  end
end
