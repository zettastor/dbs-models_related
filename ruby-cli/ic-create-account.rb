# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'information_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = InformationCenter::Client.new(protocol)

  transport.open()

  req = CreateAccountRequest.new()    
  req.accountName = ARGV[1]
  req.password = ARGV[2]
  req.accountType = ARGV[3].to_i

  pp req
  result = client.createAccount(req)
  pp result
  
  transport.close()
rescue
  puts $!
end
