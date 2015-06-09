module Purview
  module Tables
    class Base
      attr_reader :columns, :database, :indices, :name

      def initialize(name, opts={})
        @name = name
        @opts = opts
        @database = database_option
        @columns = Set.new.tap do |result|
          (default_columns + columns_option).each do |column|
            column.table = self if result.add?(column)
          end
        end
        @indices = Set.new.tap do |result|
          (default_indices + indices_option).each do |index|
            index.table = self if result.add?(index)
          end
        end
      end

      def column_names
        columns.map(&:name)
      end

      def columns_by_name
        columns.reduce({}) do |memo, column|
          memo[column.name] = column
          memo
        end
      end

      def database=(value)
        raise Purview::Exceptions::DatabaseAlreadyAssignedForTable.new(self) if database
        @database = value
      end

      private

      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts

      def columns_option
        opts[:columns] || []
      end

      def database_option
        opts[:database]
      end

      def default_columns
        []
      end

      def default_indices
        []
      end

      def indices_option
        opts[:indices] || []
      end
    end
  end
end
