module Purview
  module Types
    class Integer < Base
      def self.parse(value)
        Integer(value)
      end
    end
  end
end
