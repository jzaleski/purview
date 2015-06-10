module Purview
  module Columns
    class Base
      attr_reader :name, :table

      def initialize(name, opts={})
        @name = name.to_sym
        @opts = default_opts.merge(opts)
        @table = table_opt
      end

      def default
        opts[:default]
      end

      def default?
        !!default
      end

      def eql?(other)
        self.class == other.class &&
          limit == other.limit &&
          name == other.name &&
          nullable == other.nullable &&
          primary_key == other.primary_key &&
          type == other.type
      end

      def hash
        default.hash +
          limit.hash +
          name.hash +
          nullable.hash +
          primary_key.hash +
          type.hash
      end

      def limit
        opts[:limit]
      end

      def limit?
        !!limit
      end

      def nullable
        [nil, true].include?(opts[:nullable])
      end

      def nullable?
        !!nullable
      end

      def parse(value)
        blank = blank?(value)
        raise %{Unexpected blank value for column: "#{name}"} if blank && !nullable?
        blank ? nil : type.parse(value)
      end

      def primary_key
        opts[:primary_key]
      end

      def primary_key?
        !!primary_key
      end

      def source_name
        (opts[:source_name] || name).to_sym
      end

      def table=(value)
        raise Purview::Exceptions::TableAlreadyAssignedForColumn.new(self) if table
        @table = value
      end

      def type
        opts[:type] || Purview::Types::String
      end

      private

      include Purview::Mixins::Helpers

      attr_reader :opts

      def default_opts
        {}
      end

      def table_opt
        opts[:table]
      end
    end
  end
end
