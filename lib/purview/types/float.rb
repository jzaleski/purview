module Purview
  module Types
    class Float < Base
      def self.parse(value)
        Float(value)
      end
    end
  end
end
