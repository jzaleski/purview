module Purview
  module Parsers
    class CSV < Base
      def parse(data)
        with_context_logging("`parse` for: #{table.name}") do
          [].tap do |result|
            headers = extract_headers(data)
            extract_rows(data).each do |row|
              result << build_result(headers.zip(row))
            end
          end
        end
      end

      def validate(data)
        with_context_logging("`validate` for: #{table.name}") do
          missing_columns = missing_columns(data)
          raise 'Missing one or more columns: "%s"' % missing_columns.join('", "') \
            unless missing_columns.empty?
          true
        end
      end

      private

      def build_result(row)
        {}.tap do |result|
          row.each do |key, value|
            if column = table.columns_by_source_name[key]
              result[column.name] = column.parse(value)
            else
              logger.debug(%{Unexpected column: "#{key}" in data-set})
            end
          end
        end
      end

      def column_separator
        ','
      end

      def extract_headers(data)
        header_row = data.split(row_separator).first
        parse_row(header_row).map(&:to_sym)
      end

      def extract_rows(data)
        rows = data.split(row_separator)[1..-1]
        rows.map { |row| parse_row(row) }
      end

      def map_headers(headers)
        headers.map do |header|
          if column = table.columns_by_source_name[header]
            column.name
          else
            logger.debug(%{Could not find column with source_name: "#{header}"})
          end
        end
      end

      def missing_columns(data)
        table.column_names - map_headers(extract_headers(data))
      end

      def parse_row(row)
        ::CSV.parse(row, :col_sep => column_separator).first
      end

      def row_separator
        $/
      end
    end
  end
end
