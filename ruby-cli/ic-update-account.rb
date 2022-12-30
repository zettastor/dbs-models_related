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

  req = UpdateAccountRequest.new()
  req.accountName = ARGV[1]
  req.oldPassword = ARGV[2]
  req.newPassword = ARGV[3]
  req.accountId = ARGV[4].to_i

  pp req
  result = client.updateAccount(req)
  pp result
  
  transport.close()
rescue
  puts $!
end
