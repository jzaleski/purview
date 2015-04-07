module Purview
  module Types
    class Timestamp < Base
      def self.parse(value)
        ::Time.parse(value)
      end
    end
  end
end
