module Purview
  module Indices
    class Base
      attr_reader :columns, :table

      def initialize(columns, opts={})
        @columns = columns
        @opts = opts
        @table = table_option
      end

      def eql?(other)
        self.class == other.class &&
          columns == other.columns &&
          unique == other.unique
      end

      def hash
        columns.hash + unique.hash
      end

      def table=(value)
        raise Purview::Exceptions::TableAlreadyAssignedForIndex.new(self) if table
        @table = value
      end

      def unique
        opts[:unique]
      end

      def unique?
        !!unique
      end

      private

      attr_reader :opts

      def table_option
        opts[:table]
      end
    end
  end
end
