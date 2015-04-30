module Purview
  module Exceptions
    class BaseTable < Base
      def initialize(table)
        @table = table
      end

      private

      attr_reader :table
    end
  end
end
