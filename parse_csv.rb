require 'csv'

text = File.read('/home/dale/CSC718/Project/new_data.csv')

replace = text.delete('"')

File.open('/home/dale/CSC718/Project/new_data.csv', "w") { |file| file.puts replace }

