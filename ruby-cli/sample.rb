$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'information_center'
require 'control_center'
require 'pp'

begin

  seed = Time.now.to_i.to_s
  
  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', 8020))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = InformationCenter::Client.new(protocol)

  transport.open()
  
  puts '*' * 20 + 'Creating super admin account'
  
  req = CreateAccountRequest.new()    
  req.accountName = 'super-admin-account' + seed
  req.password = '123456'
  req.accountType = 1

  pp req
  result1 = client.createAccount(req)
  pp result1
  
  puts '*' * 20 + 'Creating admin account'
  
  req = CreateAccountRequest.new()    
  req.accountName = 'admin-account' + seed
  req.password = '123456'
  req.accountType = 2
  pp req
  result2 = client.createAccount(req)
  pp result2

  puts '*' * 20 + 'Creating regular account1'

  req3 = CreateAccountRequest.new()    
  req3.accountName = 'regular-account1' + seed
  req3.password = '123456'
  req3.accountType = 3
  pp req3
  result3 = client.createAccount(req3)
  pp result3
  
  puts '*' * 20 + 'Creating regular account2'

  req = CreateAccountRequest.new()    
  req.accountName = 'regular-account2' + seed
  req.password = '123456'
  req.accountType = 3
  pp req
  result4 = client.createAccount(req)
  pp result4
  
  puts '*' * 20 + 'Listing accounts with admin account'
  
  # only admin account can list accounts
  req = ListAccountsRequest.new()
  req.accountId = result2.accountId

  pp req
  result = client.listAccounts(req)
  pp result
  
  puts '*' * 20 + 'Listing accounts with regular account, expect to get AccessDeniedException'
  
  begin
    req = ListAccountsRequest.new()
    req.accountId = result3.accountId
    pp req
    result = client.listAccounts(req)
    pp result
  rescue Exception => e
    puts e
  end
  
  puts '*' * 20 + 'Authenticating regular account'
  
  req = AuthenticateAccountRequest.new()    
  req.accountName = req3.accountName
  req.password = req3.password

  pp req
  result = client.authenticateAccount(req)
  pp result
  
  puts '*' * 20 + 'Authenticating regular account with wrong password, expect to get Exception'
  
  begin
    req = AuthenticateAccountRequest.new()    
    req.accountName = req3.accountName
    req.password = req3.password + 'wrong'

    pp req
    result = client.authenticateAccount(req)
    pp result
  rescue Exception => e
    puts e
  end
  
  puts '*' * 20 + 'Updating account by owner'
  
  req = UpdateAccountRequest.new()
  req.accountName = 'regular-account1' + seed
  req.oldPassword = '123456'
  req.newPassword = '789012'
  req.accountId = result3.accountId

  pp req
  result = client.updateAccount(req)
  pp result
  
  puts '*' * 20 + 'Updating account by admin'
  
  req = UpdateAccountRequest.new()
  req.accountName = 'regular-account1' + seed
  req.newPassword = '345678'
  req.accountId = result2.accountId

  pp req
  result = client.updateAccount(req)
  pp result
  
  puts '*' * 20 + 'Updating account by other regular user, expecting to get AccessDeniedException'
  
  begin
    req = UpdateAccountRequest.new()
    req.accountName = 'regular-account1' + seed
    req.oldPassword = '123456'
    req.newPassword = '789012'
    req.accountId = result4.accountId

    pp req
    result = client.updateAccount(req)
    pp result
  rescue Exception => e
    puts e
  end
  
  transport.close()
  
  # control center APIs
  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', 8010))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = ControlCenter::Client.new(protocol)

  transport.open()
  
  puts '*' * 20 + 'Creating volume with regular account'
  
  req = CreateVolumeRequest.new()    
  req.name = 'vol' + seed
  req.volumeSize = 16 * (2**10)**2
  req.volumeType = 1
  req.accountId = result3.accountId

  pp req
  createVolumeResult = client.createVolume(req)
  pp createVolumeResult
  
  puts '*' * 20 + 'Sleeping 60s'
  
  sleep 60
  
  puts '*' * 20 + 'Listing volumes with regualr account, expect to get back 1 volume'
  
  req = ListVolumesRequest.new()    
  req.accountId = result3.accountId

  pp req
  result = client.listVolumes(req)
  pp result
  
  puts '*' * 20 + 'Listing volumes with admin account, expect to get back more than 1 volume'
  
  req = ListVolumesRequest.new()    
  req.accountId = result2.accountId

  pp req
  result = client.listVolumes(req)
  pp result
  
  puts '*' * 20 + 'Getting volume with regular account'
  
  req = GetVolumeRequest.new()    
  req.volumeId = createVolumeResult.volumeId
  req.accountId = result3.accountId

  pp req
  result = client.getVolume(req)
  pp result
  
  puts '*' * 20 + 'Getting volume with admin account, which is ok'
  
  req = GetVolumeRequest.new()    
  req.volumeId = createVolumeResult.volumeId
  req.accountId = result2.accountId

  pp req
  result = client.getVolume(req)
  pp result
  
  puts '*' * 20 + 'Getting volume with regualr account2, expect to get AccessDeniedException'
  
  begin
    req = GetVolumeRequest.new()    
    req.volumeId = createVolumeResult.volumeId
    req.accountId = result4.accountId

    pp req
    result = client.getVolume(req)
    pp result
  rescue Exception => e
    puts e
  end
  
  puts '*' * 20 + 'Deleting volume (created by regualr account 1) with regular account2, expect to get AccessDeniedException'
  
  begin
    req = DeleteVolumeRequest.new()    
    req.volumeId = createVolumeResult.volumeId
    req.accountId = result4.accountId

    pp req
    result = client.deleteVolume(req)
    pp result
  rescue Exception => e
    puts e
  end
  
  puts '*' * 20 + 'Deleting volume (created by regualr account 1) with regular account1'

  req = DeleteVolumeRequest.new()    
  req.volumeId = createVolumeResult.volumeId
  req.accountId = result3.accountId

  pp req
  result = client.deleteVolume(req)
  pp result

  puts '*' * 20 + 'Sleeping 60s'
  sleep 60

  puts '*' * 20 + 'Getting volume again with regular account'
  
  req = GetVolumeRequest.new()    
  req.volumeId = createVolumeResult.volumeId
  req.accountId = result3.accountId

  pp req
  result = client.getVolume(req)
  pp result
  
  transport.close()
rescue
  puts $!
end