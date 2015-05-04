module Purview
  module Parsers
    class SQL < Base
      def parse(data)
        data.rows
      end

      def validate(data)
        true
      end
    end
  end
end
