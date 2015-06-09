module Purview
  module Tables
    class BaseSyncable < Base
      def created_timestamp_column
        column_from_opts_of_type(Purview::Columns::CreatedTimestamp) or raise %{Must specify a column of type: "#{Purview::Columns::CreatedTimestamp}"}
      end

      def created_timestamp_index
        Purview::Indices::Simple.new(created_timestamp_column)
      end

      def id_column
        column_from_opts_of_type(Purview::Columns::Id) or raise %{Must specify a column of type: "#{Purview::Columns::Id}"}
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
        column_from_opts_of_type(Purview::Columns::UpdatedTimestamp) or raise %{Must specify a column of type: "#{Purview::Columns::UpdatedTimestamp}"}
      end

      def updated_timestamp_index
        Purview::Indices::Simple.new(updated_timestamp_column)
      end

      def window_size
        opts[:window_size] || (60 * 60)
      end

      private

      def column_from_opts_of_type(type)
        columns_opt.select { |column| column.is_a?(type) }.first
      end

      def default_columns
        super + [
          id_column,
          created_timestamp_column,
          updated_timestamp_column,
        ]
      end

      def default_indices
        super + [
          created_timestamp_index,
          updated_timestamp_index,
        ]
      end

      def extract_type_opt(opts)
        opts[:type]
      end

      def filter_type_opt(opts)
        opts.select { |key| key != :type }
      end

      def loader
        loader_type.new(loader_opts)
      end

      def loader_opts
        merge_table_opt(filter_type_opt(opts[:loader]))
      end

      def loader_type
        extract_type_opt(opts[:loader])
      end

      def merge_table_opt(opts)
        { :table => self }.merge(opts)
      end

      def parser
        parser_type.new(parser_opts)
      end

      def parser_opts
        merge_table_opt(filter_type_opt(opts[:parser]))
      end

      def parser_type
        extract_type_opt(opts[:parser])
      end

      def puller
        puller_type.new(puller_opts)
      end

      def puller_opts
        merge_table_opt(filter_type_opt(opts[:puller]))
      end

      def puller_type
        extract_type_opt(opts[:puller])
      end
    end
  end
end
