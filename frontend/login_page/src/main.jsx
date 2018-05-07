import {Config, CognitoIdentityCredentials} from "aws-sdk";
import {
  CognitoUserPool,
  CognitoUserAttribute,
  CognitoUser,
  AuthenticationDetails
} from "amazon-cognito-identity-js";
import React from "react";
import ReactDOM from "react-dom";
import appConfig from "./config";

import { authUser } from "./libs/awsLib";
import { apigClientFactory } from "aws-api-gateway-client"

Config.region = appConfig.region;
Config.credentials = new CognitoIdentityCredentials({
  IdentityPoolId: appConfig.IdentityPoolId
});

const userPool = new CognitoUserPool({
  UserPoolId: appConfig.UserPoolId,
  ClientId: appConfig.ClientId,
});


class SignUpForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      email: '',
      password: '',
      username: '',
      isAuthenticated: false,
      isAuthenticating: true
    };
  }

  async componentDidMount() {
    try {
      if (await authUser()) {
        this.userHasAuthenticated(true);
      }
    }
    catch(e) {
      alert(e);
    }
  
    this.setState({ isAuthenticating: false });
  }
    
  userHasAuthenticated = authenticated => {
    this.setState({ isAuthenticated: authenticated });
  }

  handleEmailChange(e) {
    this.setState({email: e.target.value});
  }

  handleUsernameChange(e) {
    this.setState({username: e.target.value});
  }

  handlePasswordChange(e) {
    this.setState({password: e.target.value});
  }

  handleSubmitSignup(e) {
    e.preventDefault();
    const email = this.state.email.trim();
    const password = this.state.password.trim();
    const attributeList = [
      new CognitoUserAttribute({
        Name: 'email',
        Value: email,
      })
    ];
    userPool.signUp(email, password, attributeList, null, (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      console.log('user name is ' + result.user.getUsername());
      console.log('call result: ' + result);
    });
  }

  handleSubmitSignIn(e) {
    e.preventDefault();
    const username = this.state.username.trim();
    const password = this.state.password.trim();

    const user = new CognitoUser({ Username: username, Pool: userPool });
    const authenticationData = { Username: username, Password: password };
    const authenticationDetails = new AuthenticationDetails(authenticationData);
  
    user.authenticateUser(authenticationDetails, {
      onSuccess: result => {
        console.log('Result: '+ JSON.stringify(result));
        alert("Logged in");
      },
      onFailure: err => {
        console.log('Failure: '+ err);
        alert(err);
      }
    })
  }

  handleLogout = event => {
    this.userHasAuthenticated(false);
  }

  handleStrucaElBotton = event => {
    let config = {invokeUrl:'https://tzvlx7065k.execute-api.us-west-2.amazonaws.com'};
    var apigClient = apigClientFactory.newClient(config);
    var params = {
        //This is where any header, path, or querystring request params go. The key is the parameter named as defined in the API
        //userId: '1234',
    };
    // Template syntax follows url-template https://www.npmjs.com/package/url-template
    var pathTemplate = '/test'
    var method = 'GET';
    var additionalParams = {
        //If there are any unmodeled query parameters or headers that need to be sent with the request you can add them here
        /*
        headers: {
            param0: '',
            param1: ''
        },
        queryParams: {
            param0: '',
            param1: ''
        }
        */
    };
    var body = {
        //This is where you define the body of the request
    };
    
    apigClient.invokeApi(params, pathTemplate, method, additionalParams, body)
        .then(function(result){
            console.log(result);
            alert(result);
        }).catch( function(result){
            console.log(result);
            alert(result);
        });
  }

  render() {
    return (
      <div>
        <form onSubmit={this.handleSubmitSignIn.bind(this)}>
          <input type="text"
                value={this.state.username}
                placeholder="User"
                onChange={this.handleUsernameChange.bind(this)}/>
          <input type="password"
                value={this.state.password}
                placeholder="Password"
                onChange={this.handlePasswordChange.bind(this)}/>
          <input type="submit"/>
        </form>
        <br/>
        <form onSubmit={this.handleSubmitSignup.bind(this)}>
          <input type="text"
                value={this.state.email}
                placeholder="Email"
                onChange={this.handleEmailChange.bind(this)}/>
          <input type="password"
                value={this.state.password}
                placeholder="Password"
                onChange={this.handlePasswordChange.bind(this)}/>
          <input type="submit"/>
        </form>

        {this.state.isAuthenticated
          ? <div><button onClick={this.handleLogout}>Logout</button><br/><button onClick={this.handleStrucaElBotton}>GO!</button></div>
          : null
        }
      </div>
    );
  }
}

ReactDOM.render(<SignUpForm />, document.getElementById('app'));

