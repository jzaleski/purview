module Purview
  module Types
    class Date < Base
      def self.parse(value)
        ::Date.parse(value)
      end
    end
  end
end
