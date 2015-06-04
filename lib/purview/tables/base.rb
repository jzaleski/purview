module Purview
  module Tables
    class Base
      attr_reader :database, :indices, :name

      def initialize(name, opts={})
        @name = name
        @opts = opts
        @indices = Set.new.tap do |result|
          ((opts[:indices] || []) + default_indices).each do |index|
            index.table = self if result.add?(index)
          end
        end
      end

      def columns
        opts[:columns]
      end

      def column_names
        columns.map(&:name)
      end

      def columns_by_name
        {}.tap do |result|
          columns.each do |column|
            result[column.name] = column
          end
        end
      end

      def columns_of_type(type)
        columns.select { |column| column.is_a?(type) }
      end

      def created_timestamp_column
        columns_of_type(Purview::Columns::CreatedTimestamp).first
      end

      def created_timestamp_index
        Purview::Indices::Simple.new(created_timestamp_column)
      end

      def data_columns
        columns - [
          created_timestamp_column,
          id_column,
          updated_timestamp_column,
        ]
      end

      def database=(value)
        raise Purview::Exceptions::DatabaseAlreadyAssigned.new(self) if database
        @database = value
      end

      def id_column
        columns_of_type(Purview::Columns::Id).first
      end

      def sync(connection, window)
        raw_data = puller.pull(window)
        parser.validate(raw_data)
        parsed_data = parser.parse(raw_data)
        loader.load(
          connection,
          parsed_data,
          window
        )
      end

      def temporary_name
        "#{name}_#{timestamp.to_i}"
      end

      def updated_timestamp_column
        columns_of_type(Purview::Columns::UpdatedTimestamp).first
      end

      def updated_timestamp_index
        Purview::Indices::Simple.new(updated_timestamp_column)
      end

      def window_size
        opts[:window_size] || (60 * 60)
      end

      private

      include Purview::Mixins::Helpers
      include Purview::Mixins::Logger

      attr_reader :opts

      def default_indices
        [
          created_timestamp_index,
          updated_timestamp_index,
        ]
      end

      def extract_type_option(opts)
        opts[:type]
      end

      def filter_type_option(opts)
        opts.select { |key| key != :type }
      end

      def loader
        loader_type.new(loader_opts)
      end

      def loader_opts
        merge_table_option(filter_type_option(opts[:loader]))
      end

      def loader_type
        extract_type_option(opts[:loader])
      end

      def merge_table_option(opts)
        { :table => self }.merge(opts)
      end

      def parser
        parser_type.new(parser_opts)
      end

      def parser_opts
        merge_table_option(filter_type_option(opts[:parser]))
      end

      def parser_type
        extract_type_option(opts[:parser])
      end

      def puller
        puller_type.new(puller_opts)
      end

      def puller_opts
        merge_table_option(filter_type_option(opts[:puller]))
      end

      def puller_type
        extract_type_option(opts[:puller])
      end
    end
  end
end
