module Purview
  module Types
    class Time < Base
      def self.parse(value)
        ::Time.parse(value)
      end
    end
  end
end
