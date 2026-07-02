Run test_contract.py first to generate a pact contract json file.
```bash
pytest test_contract.py
```


Then, run the following command to verify the contract against the provider:

```bash
pytest test_provider.py
``` 


### Running e2e kafka tests using FastStream TestKafkaBroker
Run the following test to verify the messages passed between two separate faststream application
```bash
pytest test_e2e_integration.py -vvv -s
```