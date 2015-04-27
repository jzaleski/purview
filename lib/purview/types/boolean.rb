module Purview
  module Types
    class Boolean < Base
      def self.parse(value)
        !!(value =~ /\A(true|t|yes|y|1)\z/i)
      end
    end
  end
end
