. "$PSScriptRoot/common.ps1"
Push-Location $script:LabRoot
try {
    Invoke-LabCommand mvn @('-pl', 'payment-service', 'spring-boot:run', '-Dspring-boot.run.profiles=lab')
} finally { Pop-Location }
