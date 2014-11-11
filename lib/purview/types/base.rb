module Purview
  module Types
    class Base
      def self.parse(value)
        raise %{All "#{Base}(s)" must override the "parse" method}
      end
    end
  end
end
